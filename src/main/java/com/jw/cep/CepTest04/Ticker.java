package com.jw.cep.CepTest04;

import java.io.Serializable;

public class Ticker implements Serializable {

    private String symbol;
    private String rowTime;
    private Integer price;
    private Integer tax;

    public Ticker() {
    }

    public Ticker(String symbol, String rowTime, Integer price, Integer tax) {
        this.symbol = symbol;
        this.rowTime = rowTime;
        this.price = price;
        this.tax = tax;
    }

    @Override
    public String toString() {
        return "Ticker{" +
                "symbol='" + symbol + '\'' +
                ", rowTime='" + rowTime + '\'' +
                ", price=" + price +
                ", tax=" + tax +
                '}';
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getRowTime() {
        return rowTime;
    }

    public void setRowTime(String rowTime) {
        this.rowTime = rowTime;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    public Integer getTax() {
        return tax;
    }

    public void setTax(Integer tax) {
        this.tax = tax;
    }
}
