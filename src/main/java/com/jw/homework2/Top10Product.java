package com.jw.homework2;

public class Top10Product {

    private Long productId;

    private String name;

    private Long sum;

    public Top10Product(Long productId, String name, Long sum) {
        this.productId = productId;
        this.name = name;
        this.sum = sum;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "Top10Product{" +
                "productId=" + productId +
                ", name='" + name + '\'' +
                ", sum=" + sum +
                '}';
    }
}
