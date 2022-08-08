package com.spark.scala.practice.example;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Employee {
    private String name;
    private String gender;
    private Integer depId;
    private Double salary;
    private Integer age;
}
