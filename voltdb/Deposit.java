import org.voltdb.*;

public class Deposit extends VoltProcedure {
    public final SQLStmt findCurrent = new SQLStmt(
    " SELECT value FROM kv WHERE key=?;"
    );
    public final SQLStmt updateExisting = new SQLStmt(
        " UPDATE kv SET value=?"
        + " WHERE key=?;"
    );
    public final SQLStmt addNew = new SQLStmt("INSERT INTO kv VALUES (?,?);");

    public VoltTable[] run(String to, int value) throws VoltAbortException {
        voltQueueSQL(findCurrent, to);
        VoltTable[] results = voltExecuteSQL();

        Long balance = 100l;
        if (results[0].getRowCount() <= 0 ) {
              voltQueueSQL(addNew, to, balance);
        } else {
              balance = results[0].fetchRow(0).getLong(0);
        }

        voltExecuteSQL();

        Long newBalance = balance + value;
        if (newBalance < 0) {
            throw new VoltAbortException("Cannot go below zero");
        }

        voltQueueSQL(updateExisting, newBalance, to);
        return voltExecuteSQL();
    }
}
