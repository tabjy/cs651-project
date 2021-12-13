package ca.uwaterloo.cs451.project;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;

public class Test {
    public static void main(String[] args) {
        MainNetParams np = new MainNetParams();
        new Context(np); // XXX: need to construct context before parsing



        List<File> files = new ArrayList<>();
        files.add(new File("/home/tabjy/.bitcoin/blocks/blk01500.dat"));

        files = BlockFileLoader.getReferenceClientBlockFileList();

        BlockFileLoader loader = new BlockFileLoader(np, files);

        for (Block block : loader) {
            for (Transaction transaction : block.getTransactions()) {
                for (TransactionOutput output : transaction.getOutputs()) {
                    if (transaction.isCoinBase()) {
                        output.getAddressFromP2SH(np);
                        output.getAddressFromP2PKHScript(np);
                    }
                }
            }
        }
    }
}
