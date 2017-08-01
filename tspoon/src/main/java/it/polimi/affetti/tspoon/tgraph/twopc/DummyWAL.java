package it.polimi.affetti.tspoon.tgraph.twopc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by affo on 31/07/17.
 */
public class DummyWAL implements WAL {
    private String fileName;
    private File wal;
    private FileWriter out;

    public DummyWAL(String fileName) {
        this.fileName = fileName;
    }

    public void open() throws IOException {
        wal = new File(fileName);
        wal.createNewFile();
        out = new FileWriter(wal, false);
    }

    public void close() throws IOException {
        out.close();
    }

    @Override
    public void replay(int tid) throws IOException {
        out.write("R" + tid + ";");
        out.flush();
    }

    @Override
    public void abort(int tid) throws IOException {
        out.write("A" + tid + ";");
        out.flush();
    }

    @Override
    public void commit(int tid, String updates) throws IOException {
        out.write("C" + tid + "-" + updates + ";");
        out.flush();
    }
}
