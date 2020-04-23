package com.neo.controller;

import com.neo.model.Customer;
import com.neo.service.CustomersInterface;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;

/**
 * describe:
 *
 * @author huxingjie
 * @date 2020/04/21
 * @since 4.0
 */
@Slf4j
@RestController
public class EsController {

    @Autowired
    private CustomersInterface customersInterface;

    @RequestMapping("/es")
    public Page<Customer> getResult() {
        Page<Customer> customers = customersInterface.searchCustromer(0, 10, "");
        return customers;
    }

    @RequestMapping("/func")
    public Page<Customer> getFuncResult() {
        Page<Customer> customers = customersInterface.searchCity(0, 10, "");
        return customers;
    }

    @RequestMapping("/sear")
    public Page<Customer> sear() {
        Page<Customer> customers = customersInterface.searchCustromer();
        return customers;
    }

    @RequestMapping("/update")
    public Boolean update() throws Exception {
        return customersInterface.updateCustomer();
    }
}

    