package com.example.billing;

/** Money helpers: rounding and formatting of cent amounts. */
public final class Money {
    private Money() {}

    public static long roundCents(double amount) {
        return Math.round(amount * 100);
    }

    public static String format(long cents) {
        long c = roundCents(cents / 100.0);
        return "$" + (c / 100) + "." + String.format("%02d", c % 100);
    }
}
