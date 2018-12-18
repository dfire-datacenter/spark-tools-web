package com.dfire.config;

import com.dfire.products.util.PropertyFileUtil;
import org.springframework.stereotype.Service;

/**
 * @Description :
 * @Author ： HeGuoZi
 * @Date ： 11:36 2018/12/5
 * @Modified :
 */
@Service
public class StartBean {

    public void init() {
        //设置环境变量
        if (!PropertyFileUtil.isLoaded()) {
            PropertyFileUtil.init();
        }
    }
}
