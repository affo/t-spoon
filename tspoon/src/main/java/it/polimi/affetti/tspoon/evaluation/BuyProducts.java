package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.*;
import it.polimi.affetti.tspoon.tgraph.db.ObjectHandler;
import it.polimi.affetti.tspoon.tgraph.state.StateFunction;
import it.polimi.affetti.tspoon.tgraph.state.StateStream;
import it.polimi.affetti.tspoon.tgraph.state.Update;
import it.polimi.affetti.tspoon.tgraph.twopc.OpenStream;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.*;

/**
 * Created by affo on 29/07/17.
 */
public class BuyProducts {
    public static final String TRANSACTION_TRACKING_SERVER_NAME = "transactions-tracker";
    public static final int NUMBER_OF_CUSTOMERS = 10000;
    public static final String CUSTOMER_PREFIX = "customer";
    public static final int NUMBER_OF_PRODUCTS = 100;
    public static final String PRODUCT_PREFIX = "product";
    public static final int NUMBER_OF_CATEGORIES = 5;
    public static final String CATEGORY_PREFIX = "category";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setBufferTimeout(0);
        env.getConfig().setLatencyTrackingInterval(-1);
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final int par = parameters.getInt("par", 4);
        final int partitioning = parameters.getInt("partitioning", 4);
        final boolean optimisticOrPessimistic = parameters.getBoolean("optOrNot", true);
        final int isolationLevelNumber = parameters.getInt("isolationLevel", 3);

        final int batchSize = parameters.getInt("batchSize", 1000);
        final int resolution = parameters.getInt("resolution", 100);
        final int startInputRate = parameters.getInt("startInputRate", 100);

        NetUtils.launchJobControlServer(parameters);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(par);

        final Strategy strategy = optimisticOrPessimistic ? Strategy.OPTIMISTIC : Strategy.PESSIMISTIC;
        final IsolationLevel isolationLevel = IsolationLevel.values()[isolationLevelNumber];

        TransactionEnvironment tEnv = TransactionEnvironment.get(env);
        tEnv.configIsolation(strategy, isolationLevel);
        tEnv.setSynchronous(false);
        tEnv.setDurable(false);
        tEnv.setStateServerPoolSize(Runtime.getRuntime().availableProcessors());

        PurchaseSource purchaseSource = new PurchaseSource(
                startInputRate, resolution, batchSize, TRANSACTION_TRACKING_SERVER_NAME);
        DataStream<PurchaseID> purchaseIds = env.addSource(purchaseSource).name("PurchaseIDSource");
        DataStream<Purchase> purchases = purchaseIds.map(new ToPurchase()).name("ToPurchases");

        OpenStream<Purchase> open = tEnv.open(purchases);

        TStream<Purchase> tPurchases = open.opened;

        KeySelector<Purchase, String> byUserSelector = p -> p.customer.toString();
        KeySelector<Purchase, String> byProductSelector = p -> p.product.toString();

        OutputTag<Update<List<Product>>> productsByUserUpdates =
                new OutputTag<Update<List<Product>>>("productsByUserUpdates") {
                };
        OutputTag<Update<Integer>> warehouseUpdates =
                new OutputTag<Update<Integer>>("productsUpdates") {
                };

        StateStream<Purchase, List<Product>> productsByUser = tPurchases.state(
                "productsByUser",
                productsByUserUpdates,
                byUserSelector,
                new PurchasesState(),
                partitioning
        );

        StateStream<Purchase, Integer> warehouse = tPurchases.state(
                "warehouse",
                warehouseUpdates,
                byProductSelector,
                new WarehouseState(),
                partitioning
        );

        DataStream<TransactionResult<Purchase>> transactionResults = tEnv
                .close(productsByUser.leftUnchanged, warehouse.leftUnchanged)
                .get(0);

        // --------------- Tracking
        transactionResults.map(tr -> tr.f2.id).returns(PurchaseID.class)
                .addSink(new FinishOnBackPressure<>(
                        0.25, batchSize, startInputRate,
                        resolution, -1, TRANSACTION_TRACKING_SERVER_NAME))
                .name("FinishOnBackPressure")
                .setParallelism(1);


        // --------------- Analytics
        // prints stats about the last 20 seconds about transactions
        transactionResults
                .timeWindowAll(Time.seconds(20))
                .apply(new AllWindowFunction<TransactionResult<Purchase>, Object, TimeWindow>() {
                    @Override
                    public void apply(
                            TimeWindow timeWindow,
                            Iterable<TransactionResult<Purchase>> results,
                            Collector<Object> collector) throws Exception {
                        int commits = 0;
                        int aborts = 0;

                        for (TransactionResult<Purchase> result : results) {
                            if (result.f1 == Vote.COMMIT) {
                                commits++;
                            } else {
                                aborts++;
                            }
                        }

                        System.out.printf(">>> COMMITS: %d, ABORTS: %d\n", commits, aborts);
                    }
                });

        DataStream<Purchase> successfulPurchases = transactionResults.map(tr -> tr.f2).returns(Purchase.class);


        // TODO we can use `purchases` or `successfulPurchases`
        // The 10 most requested products per category in the last 5 minutes every minute
        purchases.
                keyBy(purchase -> purchase.product.category)
                .timeWindow(Time.minutes(5), Time.minutes(1))
                .apply(new TopK<Product>(10) {
                    @Override
                    protected Product fromPurchase(Purchase purchase) {
                        return purchase.product;
                    }
                })
                .print();

        // The most active user per category in the last 10 minutes
        purchases.
                keyBy(purchase -> purchase.product.category)
                .timeWindow(Time.minutes(10))
                .apply(new TopK<Customer>(1) {
                    @Override
                    protected Customer fromPurchase(Purchase purchase) {
                        return purchase.customer;
                    }
                })
                .print();


        env.execute("Buying some products: " + strategy + " - " + isolationLevel);
    }

    public static class PurchaseID extends Tuple2<Integer, Long> implements UniquelyRepresentableForTracking {
        public PurchaseID() {
        }

        public PurchaseID(Integer taskID, Long incrementalID) {
            super(taskID, incrementalID);
        }

        @Override
        public String toString() {
            return f0 + "." + f1;
        }

        @Override
        public String getUniqueRepresentation() {
            return toString();
        }
    }

    public static class Customer implements Serializable {
        public String name;

        public Customer() {
        }

        public Customer(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Customer: " + name;
        }
    }

    public static class Product implements Serializable {
        public String name;
        public Category category;

        public Product() {

        }

        public Product(String name, Category category) {
            this.name = name;
            this.category = category;
        }

        @Override
        public String toString() {
            return "Product: " + name + ", " + category;
        }
    }

    public static class Category implements Serializable {
        public String name;

        public Category() {

        }

        public Category(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Customer: " + name;
        }

        // Must overrie to be a key type
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Category category = (Category) o;

            return name != null ? name.equals(category.name) : category.name == null;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    public static class Purchase implements Serializable {
        public PurchaseID id;
        public Customer customer;
        public Product product;

        public Purchase() {
        }

        public Purchase(PurchaseID id, Customer customer, Product product) {
            this.id = id;
            this.customer = customer;
            this.product = product;
        }

        @Override
        public String toString() {
            return "Purchase{" +
                    "id=" + id +
                    ", customer=" + customer +
                    ", product=" + product +
                    '}';
        }
    }

    private static class WarehouseState implements StateFunction<Purchase, Integer> {

        @Override
        public Integer defaultValue() {
            return 100;
        }

        @Override
        public Integer copyValue(Integer value) {
            return value;
        }

        @Override
        public boolean invariant(Integer units) {
            return units > 0;
        }

        @Override
        public void apply(Purchase element, ObjectHandler<Integer> handler) {
            Integer currentNumberOfUnits = handler.read();
            currentNumberOfUnits--;
            handler.write(currentNumberOfUnits);
        }
    }

    private static class PurchasesState implements StateFunction<Purchase, List<Product>> {

        @Override
        public List<Product> defaultValue() {
            return new LinkedList<>();
        }

        @Override
        public List<Product> copyValue(List<Product> value) {
            return new LinkedList<>(value);
        }

        @Override
        public boolean invariant(List<Product> value) {
            // the value is always ok
            return true;
        }

        @Override
        public void apply(Purchase element, ObjectHandler<List<Product>> handler) {
            List<Product> products = handler.read();
            products.add(element.product);
            handler.write(products);
        }
    }

    private static class PurchaseSource extends TunableSource<PurchaseID> {

        public PurchaseSource(int baseRate, int resolution, int batchSize,
                              String trackingServerNameForDiscovery) {
            super(baseRate, resolution, batchSize, trackingServerNameForDiscovery);
        }

        @Override
        protected PurchaseID getNext(int count) {
            return new PurchaseID(taskNumber, (long) count);
        }
    }

    private static class ToPurchase extends RichMapFunction<PurchaseID, Purchase> {
        private int taskID;
        private Random random;
        private final Map<String, Customer> customers = new HashMap<>();
        private final Map<String, Product> products = new HashMap<>();
        private final Map<String, Category> categories = new HashMap<>();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.taskID = getRuntimeContext().getIndexOfThisSubtask();
            this.random = new Random(taskID);
        }

        @Override
        public Purchase map(PurchaseID purchaseID) throws Exception {
            int customerIndex = random.nextInt(NUMBER_OF_CUSTOMERS);
            int productIndex = random.nextInt(NUMBER_OF_PRODUCTS);
            int categoryIndex = productIndex % NUMBER_OF_CATEGORIES;

            Customer customer = customers.computeIfAbsent(CUSTOMER_PREFIX + customerIndex, Customer::new);
            Category category = categories.computeIfAbsent(CATEGORY_PREFIX + categoryIndex, Category::new);
            Product product = products.computeIfAbsent(PRODUCT_PREFIX + productIndex, pk -> new Product(pk, category));
            return new Purchase(purchaseID, customer, product);
        }
    }

    private abstract static class TopK<T> implements WindowFunction<Purchase, T, Category, TimeWindow> {
        private final int k;

        public TopK(int k) {
            this.k = k;
        }

        @Override
        public void apply(Category category, TimeWindow timeWindow, Iterable<Purchase> purchases, Collector<T> collector) throws Exception {
            final HashMap<String, Tuple2<Integer, T>> ranking = new HashMap<>();

            for (Purchase p : purchases) {
                Tuple2<Integer, T> counter = ranking
                        .computeIfAbsent(p.product.name, pName -> Tuple2.of(0, fromPurchase(p)));
                counter.f0++;
            }

            for (int i = 0; i < k; i++) {
                Map.Entry<String, Tuple2<Integer, T>> max =
                        Collections.max(ranking.entrySet(), Comparator.comparingInt(e -> e.getValue().f0));
                ranking.remove(max.getKey());
                collector.collect(max.getValue().f1);
            }
        }

        protected abstract T fromPurchase(Purchase purchase);
    }
}
