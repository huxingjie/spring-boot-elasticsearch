package com.neo.service;

import com.neo.model.Customer;
import org.springframework.data.domain.Page;

import java.util.List;

public interface CustomersInterface {

    List<Customer> searchCity(Integer pageNumber, Integer pageSize, String searchContent);

    Page<Customer> searchCustromer(Integer pageNumber, Integer pageSize, String searchContent);

    List<Customer> searchCustromerAndOr();
}
