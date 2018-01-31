package com.kongbig.sparkproject.test;

/**
 * Describe: 单例模式
 * 希望在整个程序运行期间，只有一个实例，任何外界代码都不能随意创建实例
 * <p>
 * Author:   kongbig
 * Data:     2018/1/30.
 */
public class Singleton {

    /**
     * 1.必须有一个私有的静态变量，来引用自己即将被创建出来的单例
     */
    private static Singleton instance = null;

    /**
     * 2.构造方法私有化
     */
    private Singleton() {

    }

    /**
     * 3.公有的静态方法
     * <p>
     * 考虑多线程并发访问安全的问题
     *
     * @return
     */
    public static Singleton getInstance() {
        // 两步检查机制
        // 第一步，多个线程过来的时候，判断instance是否为null，如果为null再往下走
        if (instance == null) {
            // 在这里进行多个线程的同步
            // 同一时间，只能有一个线程获取到Singleton Class对象的锁，进入后续的代码
            // 其它线程，都是只能在原地等待，获取锁
            synchronized (Singleton.class) {
                // 只有第一个获取到锁的线程，进入到这里，会发现instance是null
                // 然后才会去创建这个单例
                // 此后，线程，哪怕是走到了这一步，也会发现instance已经不是null了
                // 就不会反复创建一个单例
                if (instance == null) {
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }

}
