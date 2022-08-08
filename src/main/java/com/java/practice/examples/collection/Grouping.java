package com.java.practice.examples.collection;

import com.java.practice.examples.collection.model.Employee;

import java.util.stream.Collectors;

public class Grouping {
    public static void main(String[] args){
        CollectionsSortExample.empList.get()
                .stream()
                .collect(Collectors.groupingBy(Employee::getGender,))
    }
}
