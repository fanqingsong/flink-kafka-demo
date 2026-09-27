package org.example.web;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

final class PurchaseMessages {

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'.000000'").withZone(ZoneOffset.UTC);
    private static final BigDecimal MEMBER_RATE = new BigDecimal("0.10");

    private PurchaseMessages() {
    }

    static void checkQuantity(int quantity) {
        if (quantity < 1 || quantity > 99) {
            throw new ApiException(400, "quantity must be between 1 and 99");
        }
    }

    static Map<String, Object> build(String productId,
                                      String productName,
                                      BigDecimal price,
                                      int quantity,
                                      boolean member,
                                      Instant now,
                                      String transactionId) {
        BigDecimal unit = money(price);
        BigDecimal discount = member ? money(MEMBER_RATE) : money(BigDecimal.ZERO);
        BigDecimal total = money(unit.multiply(BigDecimal.valueOf(quantity)).multiply(BigDecimal.ONE.subtract(discount)));
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("transaction_time", TIME.format(now));
        record.put("transaction_id", transactionId);
        record.put("product_id", productId);
        record.put("price", unit);
        record.put("quantity", quantity);
        record.put("is_member", member);
        record.put("member_discount", discount);
        record.put("add_supplements", false);
        record.put("supplement_price", money(BigDecimal.ZERO));
        record.put("total_purchase", total);
        record.put("product_name", productName);
        return record;
    }

    static String transactionId(Instant now, UUID uuid) {
        return now.toEpochMilli() + "-" + uuid.toString().replace("-", "").substring(0, 8);
    }

    private static BigDecimal money(BigDecimal value) {
        return value.setScale(2, RoundingMode.HALF_UP);
    }
}
