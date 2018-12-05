package com.dfire.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Description :
 * @Author ： HeGuoZi
 * @Date ： 11:36 2018/12/5
 * @Modified :
 */
@Configuration
public class InitConfig {

    @Bean(initMethod = "init")
    public StartBean start() {
        return new StartBean();
    }

}
