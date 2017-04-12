package com.revjet.interview;

import com.revjet.interview.combiner.Combiner;
import com.revjet.interview.combiner.CombinerImpl;

/**
 * @author skovalyov
 */
public class App {

    public static void main(String[] args) throws InterruptedException {
        Combiner<Integer> c = new CombinerImpl<>();
        c.start();
        Thread.sleep(1000);
        c.stop();
    }

}
