package com.atguigu.gmall;

/**
 * ClassName: Wrapper
 * Package: com.atguigu.gmall
 * Description:
 *
 * @Author JWT
 * @Create 2025/7/22 10:01
 * @Version 1.0
 */
public class Wrapper {
    public String value;

    public Wrapper(String value) {
        this.value = value;
    }


    public static void main(String[] args) {
        Wrapper a = new Wrapper("123");
        Wrapper b = new Wrapper("aaa");
        swit(a, b);
        System.out.println(a.value); // aaa
        System.out.println(b.value); // 123
    }

    private static void swit(Wrapper a, Wrapper b) {
        String temp = a.value;
        a.value = b.value;
        b.value = temp;
    }

}
