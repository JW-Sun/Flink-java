package com.jw.homework3.entity;

public class KfkSource {
    private String base_info;
    private String userName;
    private Integer userId;
    private Integer userAge;

    public KfkSource() {
    }

    public KfkSource(String base_info, String userName, Integer userId, Integer userAge) {
        this.base_info = base_info;
        this.userName = userName;
        this.userId = userId;
        this.userAge = userAge;
    }

    public String getBase_info() {
        return base_info;
    }

    public void setBase_info(String base_info) {
        this.base_info = base_info;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getUserAge() {
        return userAge;
    }

    public void setUserAge(Integer userAge) {
        this.userAge = userAge;
    }

    @Override
    public String toString() {
        return "KfkSource{" +
                "base_info='" + base_info + '\'' +
                ", userName='" + userName + '\'' +
                ", userId=" + userId +
                ", userAge=" + userAge +
                '}';
    }
}
