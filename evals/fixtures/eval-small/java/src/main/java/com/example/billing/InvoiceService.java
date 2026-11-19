package com.example.billing;

import java.util.ArrayList;
import java.util.List;

public class InvoiceService {
    private final List<Invoice> issued = new ArrayList<>();

    public String issue(Invoice invoice) {
        audit(invoice);
        issued.add(invoice);
        return Money.format(invoice.total());
    }

    private void audit(Invoice invoice) {
        if (invoice.total() < 0) {
            throw new IllegalStateException("negative invoice");
        }
    }
}
