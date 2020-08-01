package com.jw.cep.CepTest03.pojo;

public class SubEvent extends Event {

    private String date;


    public SubEvent(String type, Integer volume, String id, String date) {
        super(type, volume, id);
        this.date = date;
    }

    @Override
    public String toString() {
        return "SubEvent{" +
                "date='" + date + '\'' +
                '}';
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
