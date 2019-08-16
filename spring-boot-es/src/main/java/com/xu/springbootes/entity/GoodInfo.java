package com.xu.springbootes.entity;


import com.xu.core.annotation.ElasticId;
import com.xu.core.annotation.ElasticIndex;

import java.io.Serializable;


@ElasticIndex(index = "testgood",type = "good")
public class GoodInfo implements Serializable {
    @ElasticId
    private Long id;
    private String name;
    private String description;
    private String description1;

    public String getDescription1() {
        return description1;
    }

    public void setDescription1(String description1) {
        this.description1 = description1;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public GoodInfo(Long id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    public GoodInfo() {
    }
}

