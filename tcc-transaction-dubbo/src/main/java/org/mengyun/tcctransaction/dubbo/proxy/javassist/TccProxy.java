/*
 * Copyright 1999-2011 Alibaba Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mengyun.tcctransaction.dubbo.proxy.javassist;

import com.alibaba.dubbo.common.utils.ClassHelper;
import com.alibaba.dubbo.common.utils.ReflectUtils;
import org.mengyun.tcctransaction.api.Compensable;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TccProxy.
 *
 * @author qian.lei
 *
 * 核心目的：在原有dubbo生成Proxy时，将Tcc框架的信息加入proxy中，以用于传递信息
 */

public abstract class TccProxy {
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);

    private static final String PACKAGE_NAME = TccProxy.class.getPackage().getName();

    public static final InvocationHandler RETURN_NULL_INVOKER = new InvocationHandler() {
        public Object invoke(Object proxy, Method method, Object[] args) {
            return null;
        }
    };

    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };

    private static final Map<ClassLoader, Map<String, Object>> ProxyCacheMap = new WeakHashMap<ClassLoader, Map<String, Object>>();

    private static final Object PendingGenerationMarker = new Object();

    /**
     * Get proxy.
     *
     * @param ics interface class array.
     * @return TccProxy instance.
     */
    public static TccProxy getProxy(Class<?>... ics) {
        return getProxy(ClassHelper.getCallerClassLoader(TccProxy.class), ics);
    }

    /**
     * Get proxy.
     *
     * @param cl  class loader.
     * @param ics interface class array.
     * @return TccProxy instance.
     */
    public static TccProxy getProxy(ClassLoader cl, Class<?>... ics) {
        if (ics.length > 65535)
            throw new IllegalArgumentException("interface limit exceeded");
        //（1）遍历所有入参接口，以；作为分隔符连接起来，以这个串作为key，从缓存中查找，如果有，说明代理对象已创建成功。
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ics.length; i++) {
            String itf = ics[i].getName();
            if (!ics[i].isInterface())
                throw new RuntimeException(itf + " is not a interface.");

            Class<?> tmp = null;
            try {
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            if (tmp != ics[i])
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");

            sb.append(itf).append(';');
        }

        //（1）遍历所有入参接口，以；作为分隔符连接起来，以这个串作为key，从缓存中查找，如果有，说明代理对象已创建成功。
        // use interface class name list as key.
        String key = sb.toString();

        // get cache by class loader.
        Map<String, Object> cache;
        synchronized (ProxyCacheMap) {
            cache = ProxyCacheMap.get(cl);
            if (cache == null) {
                cache = new HashMap<String, Object>();
                ProxyCacheMap.put(cl, cache);
            }
        }

        //（1）遍历所有入参接口，以；作为分隔符连接起来，以这个串作为key，从缓存中查找，如果有，说明代理对象已创建成功。
        TccProxy proxy = null;
        synchronized (cache) {
            do {
                Object value = cache.get(key);
                if (value instanceof Reference<?>) {
                    proxy = (TccProxy) ((Reference<?>) value).get();
                    if (proxy != null)
                        return proxy;
                }
                // 如果此时正在创建，则等待通知创建完成
                if (value == PendingGenerationMarker) {
                    try {
                        cache.wait();
                    } catch (InterruptedException e) {
                    }
                } else {
                    // 没有正在创建，则自己标识状态，然后开始创建
                    cache.put(key, PendingGenerationMarker);
                    break;
                }
            }
            while (true);
        }
        //（2）利用AtomicLong对象自动获取一个long数组来作为生产类的后缀，防止冲突。
        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;
        TccClassGenerator ccp = null, ccm = null;
        try {
            // 创建一个TccClassGenerator实例
            ccp = TccClassGenerator.newInstance(cl);
            // 定义两个集合
            Set<String> worked = new HashSet<String>();
            List<Method> methods = new ArrayList<Method>();
            // 这个for先忽略，加上调用方只传了一个接口类型
            for (int i = 0; i < ics.length; i++) {
                // 如果接口的修饰符是不是public，找到该接口所在包的名称
                if (!Modifier.isPublic(ics[i].getModifiers())) {
                    String npkg = ics[i].getPackage().getName();
                    if (pkg == null) {
                        pkg = npkg;
                    } else {
                        if (!pkg.equals(npkg))
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                    }
                }
                // 向TccClassGenerator中添加接口
                ccp.addInterface(ics[i]);

                // 遍历 接口的所有方法：根据接口的方法，创建对应的一套代理方法,并添加进TccClassGenerator实例中。
                for (Method method : ics[i].getMethods()) {
                    // 以接口描述为唯一标识，相同标识的方法只处理一个
                    String desc = ReflectUtils.getDesc(method);
                    if (worked.contains(desc))
                        continue;
                    worked.add(desc);
                    // 获取本次遍历的方法的 返回值、参数类型列表，后面生成动态代码
                    int ix = methods.size();
                    Class<?> rt = method.getReturnType();
                    Class<?>[] pts = method.getParameterTypes();

                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    for (int j = 0; j < pts.length; j++)
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    code.append(" Object ret = handler.invoke(this, methods[" + ix + "], args);");
                    if (!Void.TYPE.equals(rt))
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");
                    /**
                     * 生成的code内容；
                     * Object[] args = new Object[method的参数个数];
                     * args[0] = ($w)$1;
                     * ...
                     * arg[method.parameterTypes().length] = ($w)$length+1;
                     *
                     * Object ret = handler.invoke(this,method,args);// 用handler执行method自身逻辑
                     * return ret;// asArgument做空值处理
                     *
                     */
                    // 将methods加入集合
                    methods.add(method);

                    StringBuilder compensableDesc = new StringBuilder();

                    Compensable compensable = method.getAnnotation(Compensable.class);

                    // 如果发放存在注解，向ccp添加方法时，标记出来；
                    // addMethod内部实现，就是代码生成器一样，拼接出方法；
                    // 如果有@Compensable注解，则会将代理方法加入到CompensableMethods的集合
                    if (compensable != null) {
                        ccp.addMethod(true, method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                    } else {
                        ccp.addMethod(false, method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                    }
                }
            }
            // 到这里，ccp中已经包含了指定接口的所有方法的代理了。并得到了一个mehtods列表

            if (pkg == null)
                pkg = PACKAGE_NAME;

            //（2）利用AtomicLong对象自动获取一个long数组来作为生产类的后缀，防止冲突。
            //（4）创建代理实现对象ProxyInstance; 类名为：pkg+".proxy"+id=包名+”.proxy“+自增数值。添加静态字段，添加实例对象，添加构造函数。
            // create ProxyInstance class.创建代理实例类
            String pcn = pkg + ".proxy" + id;// 按接口包名或默认包名，创建一个代理类的全路径
            ccp.setClassName(pcn);
            // 定义 成员变量、带参构造函数、默认构造函数
            ccp.addField("public static java.lang.reflect.Method[] methods;");
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");// 与上面方法中的handler对应
            // 读到这里$1很突兀，猜测是形参中下标为1的参数，非静态方法下标为0是this
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");// 对成员handler赋值，只有一个参数
            ccp.addDefaultConstructor();
            Class<?> clazz = ccp.toClass();
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));// 给自定义的类中成员methods赋值为methods列表的第一个元素

            // 至此TccClassGenerator ccp的定义已完成。

            //（2）利用AtomicLong对象自动获取一个long数组来作为生产类的后缀，防止冲突。
            // create TccProxy class. 创建Tcc代理类
            String fcn = TccProxy.class.getName() + id;
            // 创建一个TccClassGenerator实例,设置类名、默认构造函数、父类
            ccm = TccClassGenerator.newInstance(cl);
            ccm.setClassName(fcn);
            ccm.addDefaultConstructor();
            ccm.setSuperClass(TccProxy.class); // 这里是来实现TccProxy抽象类的
            // 实现抽象方法newInstance(InvocationHandler handler),返回的是ccp的实例（用的pcn就是ccp的全类名）
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn + "($1); }");
            // 重点toClass()方法，在内部 生成的类，添加了Tcc-tarnsaction框架中的主要信息
            // confirm、cancel 阶段对应方法、配置信息等等；这些是通过compensableMethods收集的信息创建的
            Class<?> pc = ccm.toClass(); // 【TODO 生成Class时，将@compensable注解信息生成，并添加到对应method的代理方法中】
            proxy = (TccProxy) pc.newInstance();// 创建代理类的实例，注意：这里的newInstance与代理类中的不是同一个
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // release TccClassGenerator
            if (ccp != null)
                ccp.release();
            if (ccm != null)
                ccm.release();
            synchronized (cache) {
                if (proxy == null)
                    cache.remove(key);
                else
                    cache.put(key, new WeakReference<TccProxy>(proxy));
                cache.notifyAll();
            }
        }
        return proxy;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);

    protected TccProxy() {
    }

    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl)
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            if (Byte.TYPE == cl)
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            if (Character.TYPE == cl)
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            if (Double.TYPE == cl)
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            if (Float.TYPE == cl)
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            if (Integer.TYPE == cl)
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            if (Long.TYPE == cl)
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            if (Short.TYPE == cl)
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }
}