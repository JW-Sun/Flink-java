package com.jw.homework;

public class Order {
    private Integer id;

    private Integer consumer_id;

    private Integer product_id;

    private Integer num;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getConsumer_id() {
        return consumer_id;
    }

    public void setConsumer_id(Integer consumer_id) {
        this.consumer_id = consumer_id;
    }

    public Integer getProduct_id() {
        return product_id;
    }

    public void setProduct_id(Integer product_id) {
        this.product_id = product_id;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public Order(Integer id, Integer consumer_id, Integer product_id, Integer num) {
        this.id = id;
        this.consumer_id = consumer_id;
        this.product_id = product_id;
        this.num = num;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", consumer_id=" + consumer_id +
                ", product_id=" + product_id +
                ", num=" + num +
                '}';
    }
}
