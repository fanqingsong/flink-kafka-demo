package org.example.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class SalesController {

    private final PipelineState state;
    private final Catalog catalog;
    private final PurchaseService purchaseService;

    public SalesController(PipelineState state, Catalog catalog, PurchaseService purchaseService) {
        this.state = state;
        this.catalog = catalog;
        this.purchaseService = purchaseService;
    }

    @GetMapping("/api/state")
    public Map<String, Object> state() {
        return state.snapshot(catalog);
    }

    @PostMapping("/api/purchases")
    public Map<String, Object> send(@RequestBody PurchaseRequest request) {
        return purchaseService.send(request);
    }
}
