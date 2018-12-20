package com.dfire.products.hera;

import com.dfire.products.hera.constants.TimeFormatConstant;
import com.dfire.products.hera.util.HeraDateTool;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import java.io.StringWriter;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RenderHierarchyProperties {

    static {
        try {
            Velocity.init();
        } catch (Exception e) {
        }
    }

    static Pattern pt = Pattern.compile("\\$\\{zdt.*?\\}");


    /**
     * @param template
     * @return
     * @desc hera配置日期变量替换, 如：${zdt.addDay(-2).format("yyyyMMdd")}，${zdt.addDay(-1).format("yyyyMMdd")}
     */
    public static String render(String template) {
        if (template == null) {
            return null;
        }
        Matcher matcher = pt.matcher(template);
        while (matcher.find()) {
            String m = template.substring(matcher.start(), matcher.end());
            StringWriter sw = new StringWriter();
            try {
                VelocityContext context = new VelocityContext();
                context.put("zdt", new HeraDateTool(new Date()));
                Velocity.evaluate(context, sw, "", m);
                if (m.equals(sw.toString())) {
                    break;
                }
            } catch (Exception e) {
                break;
            }
            template = template.replace(m, sw.toString());
            matcher = pt.matcher(template);
        }
        template = template.replace("${yesterday}", new HeraDateTool(new Date()).addDay(-1).format(TimeFormatConstant.YYYYMMDD));
        return template;
    }

    /**
     * @param template
     * @param dateStr
     * @return hera配置日期变量替换,"${yesterday}"为系统变量
     */
    public static String render(String template, String dateStr) {
        if (template == null) {
            return null;
        }
        Matcher matcher = pt.matcher(template);
        while (matcher.find()) {
            String m = template.substring(matcher.start(), matcher.end());
            StringWriter sw = new StringWriter();
            try {
                VelocityContext context = new VelocityContext();
                context.put("zdt", new HeraDateTool(HeraDateTool.StringToDate(dateStr, "yyyyMMddHHmmss")));
                Velocity.evaluate(context, sw, "", m);
                if (m.equals(sw.toString())) {
                    break;
                }
            } catch (Exception e) {
                break;
            }
            template = template.replace(m, sw.toString());
            matcher = pt.matcher(template);
        }
        template = template.replace("${yesterday}", new HeraDateTool(HeraDateTool.StringToDate(dateStr, "yyyyMMddHHmmss")).addDay(-1).format("yyyyMMdd"));
        return template;
    }
}
