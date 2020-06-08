package com.paic.dpp.factory;

import com.paic.dpp.anno.ConfigurationProperties;
import com.paic.dpp.util.ClassUtil;
import com.paic.dpp.util.ResourceUtil;
import org.apache.commons.lang3.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dengyu
 * @Function: POJO工厂类，扫描POJO后自动把属性注入，并把实例存入MAP中返回
 * @date 2020/06/02
 *
 */
public class PojoFactory {
    static Logger logger = LoggerFactory.getLogger(PojoFactory.class);
    private static Map<String,Object> pojoMap = new HashMap<String,Object>();

    static{
        List<Class<?>> classList = ClassUtil.getAllClassByPkgName("com.paic.dpp.pojo");
        for (Class<?> pojoClass : classList) {
            if(pojoClass.isAnnotationPresent(ConfigurationProperties.class)){
                logger.info("Scan annotation class --  ["+pojoClass.getName()+"]");
                String prefix = pojoClass.getAnnotation(ConfigurationProperties.class).prefix();
                Field[] declaredFields = pojoClass.getDeclaredFields();
                Object o = null;
                try {
                    o = pojoClass.newInstance();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                for (Field field :declaredFields
                        ) {
                    String propertyName = prefix+"."+field.getName();
                    String propertyVal = ResourceUtil.getProperties().containsKey(propertyName)? ResourceUtil.getProperties().getProperty(propertyName): null;
                    //set
                    try {
                        Method declaredMethod = pojoClass.getDeclaredMethod("set" + WordUtils.capitalize(field.getName()),String.class);
                        declaredMethod.invoke(o, propertyVal);
                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                        continue;
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                        continue;
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                        continue;
                    }
                }
                pojoMap.put(WordUtils.uncapitalize(pojoClass.getSimpleName()),o);
            }
        }

    }

    public static Object getServiceBean(String id){
        return pojoMap.get(id);
    }
}
