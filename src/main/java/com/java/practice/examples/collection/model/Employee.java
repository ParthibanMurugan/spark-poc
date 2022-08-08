package com.java.practice.examples.collection.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Employee {
    private String name;
    private Integer age;
    private Integer salary;
    private String gender;
}
