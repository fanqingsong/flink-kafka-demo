package org.example.web;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PurchaseMessagesTest {

    private static final Instant NOW = Instant.parse("2022-09-13T12:58:36Z");

    @Test
    void nonMemberPaysListPrice() {
        Map<String, Object> record = PurchaseMessages.build(
                "SC04", "Health Nut", new BigDecimal("5.99"), 1, false, NOW, "1000-abcd1234");

        assertEquals(new BigDecimal("5.99"), record.get("price"));
        assertEquals(new BigDecimal("5.99"), record.get("total_purchase"));
        assertEquals(new BigDecimal("0.00"), record.get("member_discount"));
        assertEquals(false, record.get("is_member"));
        assertEquals("Health Nut", record.get("product_name"));
        assertEquals("2022-09-13 12:58:36.000000", record.get("transaction_time"));
    }

    @Test
    void memberGetsTenPercentOff() {
        Map<String, Object> record = PurchaseMessages.build(
                "SC04", "Health Nut", new BigDecimal("5.99"), 2, true, NOW, "1000-abcd1234");

        assertEquals(new BigDecimal("10.78"), record.get("total_purchase"));
        assertEquals(new BigDecimal("0.10"), record.get("member_discount"));
        assertEquals(true, record.get("is_member"));
    }

    @Test
    void quantityMustBeBetween1And99() {
        ApiException tooSmall = assertThrows(ApiException.class, () -> PurchaseMessages.checkQuantity(0));
        ApiException tooLarge = assertThrows(ApiException.class, () -> PurchaseMessages.checkQuantity(100));
        assertEquals(400, tooSmall.status());
        assertEquals(400, tooLarge.status());
        PurchaseMessages.checkQuantity(1);
        PurchaseMessages.checkQuantity(99);
    }
}
