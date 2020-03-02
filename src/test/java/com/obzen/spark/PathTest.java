package com.obzen.spark;

import org.junit.Test;

public class PathTest {
    @Test
    public void printAbsolutePath() {
        System.out.println(ClassLoader.getSystemResource(".").getPath());
        System.out.println(ClassLoader.getSystemResource("data/aaa.txt").getPath());
        System.out.println(System.getProperty("user.dir"));
    }
}
