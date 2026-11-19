package com.example.billing;

public class InvoiceServiceTest {
    public void testIssue() {
        InvoiceService service = new InvoiceService();
        Invoice invoice = new Invoice();
        invoice.addLine(1.0);
        String receipt = service.issue(invoice);
        if (!receipt.equals("$1.00")) {
            throw new AssertionError(receipt);
        }
    }
}
