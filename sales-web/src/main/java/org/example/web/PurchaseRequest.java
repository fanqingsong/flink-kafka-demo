package org.example.web;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PurchaseRequest {

    private String productId;
    private int quantity;
    private boolean member;

    @JsonProperty("product_id")
    public String getProductId() {
        return productId;
    }

    @JsonProperty("product_id")
    public void setProductId(String productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @JsonProperty("is_member")
    public boolean isMember() {
        return member;
    }

    @JsonProperty("is_member")
    public void setMember(boolean member) {
        this.member = member;
    }
}
