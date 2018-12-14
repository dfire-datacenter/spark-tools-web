package com.dfire.products.util;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description :
 * @Author ： HeGuoZi
 * @Date ： 4:38 PM 2018/12/14
 * @Modified :
 */
@Data
public class HeraJobEntity {

    long   id;
    int    groupId;
    String configs;
    String script;
    List<Map<String, String>> configList = new ArrayList<>();

}
