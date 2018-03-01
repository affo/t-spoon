import org.voltdb.*;

public class Transfer extends VoltProcedure {
    public final SQLStmt findCurrent = new SQLStmt(
    " SELECT value FROM kv WHERE key=?;"
    );
    public final SQLStmt updateExisting = new SQLStmt(
        " UPDATE kv SET value=?"
        + " WHERE key=?;"
    );
    public final SQLStmt addNew = new SQLStmt("INSERT INTO kv VALUES (?,?);");

    public VoltTable[] run(String from, String to, int value) throws VoltAbortException {
        voltQueueSQL(findCurrent, from);
        voltQueueSQL(findCurrent, to);
        VoltTable[] results = voltExecuteSQL();

        Long oldFrom = 100l;
        if (results[0].getRowCount() <= 0 ) {
              voltQueueSQL(addNew, from, oldFrom);
        } else {
              oldFrom = results[0].fetchRow(0).getLong(0);
        }

        Long oldTo = 100l;
        if (results[1].getRowCount() <= 0 ) {
              voltQueueSQL(addNew, to, oldTo);
        } else {
              oldTo = results[1].fetchRow(0).getLong(0);
        }

        voltExecuteSQL();

        Long newFrom = oldFrom - value;
        if (newFrom < 0) {
            throw new VoltAbortException("Cannot go below zero");
        }
        Long newTo = oldTo + value;

        voltQueueSQL(updateExisting, newFrom, from);
        voltQueueSQL(updateExisting, newTo, to);

        return voltExecuteSQL();
    }
}

