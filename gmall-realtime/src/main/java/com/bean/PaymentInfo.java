package com.bean;

import lombok.Data;

import java.math.BigDecimal;
/**
 * @author 刘帅
 * @create 2021-09-28 8:36
 */



@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}

