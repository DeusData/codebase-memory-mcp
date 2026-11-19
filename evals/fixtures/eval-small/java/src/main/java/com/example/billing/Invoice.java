package com.example.billing;

import java.util.ArrayList;
import java.util.List;

public class Invoice {
    private final List<Double> lines = new ArrayList<>();

    public void addLine(double amount) {
        lines.add(amount);
    }

    public long total() {
        long t = 0;
        for (double a : lines) {
            t += Money.roundCents(a);
        }
        return t;
    }
}
