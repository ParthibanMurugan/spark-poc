package com.java.practice.examples.collection;

import com.java.practice.examples.collection.model.Employee;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CollectionsSortExample {

    private static Comparator<Employee> empComp = (emp1, emp2)->{
        return emp2.getName().compareTo(emp1.getName());
    };
    public static Supplier<List<Employee>> empList = ()->{
      return Arrays.asList(Employee.builder()
                      .age(32)
                      .name("Parthiban")
                      .gender("Male")
                      .salary(10000)
              .build(),Employee.builder()
              .age(21)
              .name("Preethi")
                      .gender("Female")
              .salary(8000)
              .build(),Employee.builder()
              .age(2)
              .name("Thaswika")
                      .gender("Female")
              .salary(1000)
              .build(),
              Employee.builder()
                      .age(30)
                      .name("Parthiban")
                      .gender("Male")
                      .salary(9000)
                      .build());
    };

    public static void main(String[] args){
        //Function<Employee,String> getName = (emp)->emp.getName();
        Comparator<String> nameComp = (n1,n2)->n1.compareTo(n2);

       Comparator<Employee> empCompLocal =  Comparator.comparing(Employee::getName,nameComp)
                .thenComparing(Employee::getAge,(emp1,emp2)->emp1.compareTo(emp2));

        System.out.println(empList.get());

        List<Employee> list = empList.get()
                .stream()
                .sorted(empCompLocal)
                .collect(Collectors.toList());

       System.out.println(list);

    }
}
