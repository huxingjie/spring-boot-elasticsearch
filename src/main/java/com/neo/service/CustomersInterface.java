package com.neo.service;

import com.neo.model.Customer;
import org.springframework.data.domain.Page;

import java.io.IOException;
import java.util.List;

public interface CustomersInterface {

    Page<Customer> searchCity(Integer pageNumber, Integer pageSize, String searchContent);

    Page<Customer> searchCustromer();

    Page<Customer> searchCustromer(Integer pageNumber, Integer pageSize, String searchContent);

    List<Customer> searchCustromerAndOr();

    boolean updateCustomer() throws Exception;
}
