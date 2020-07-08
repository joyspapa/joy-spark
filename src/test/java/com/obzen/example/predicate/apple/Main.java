package com.obzen.example.predicate.apple;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * [출처] 자바8 코드, 메서드 전달하기, 프레디케이트(Predicate) - 기초 (1)|작성자 호식이
*  https://blog.naver.com/gngh0101/221328402797
 */
public class Main {

	public static void main(String[] args) {
		List<Apple> inventory = new ArrayList<Apple>();
		inventory.add(new Apple("GREEN", 10));
		inventory.add(new Apple("RED", 12));
		
		List<Apple> result = filterApples(inventory, Apple::isGreenApple);
		
		System.out.println(result.size());
	}

	public static List<Apple> filterApples(List<Apple> inventory, Predicate<Apple> p) {
        List<Apple> result = new ArrayList<Apple>();
        for (Apple apple : inventory) {
            if (p.test(apple)) {
                result.add(apple);
            }
        }
        return result;
    }
}
