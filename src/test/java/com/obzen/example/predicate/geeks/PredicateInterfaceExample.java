package com.obzen.example.predicate.geeks;

import java.util.Objects;
import java.util.function.Predicate;

import org.junit.Test;

/**
 * https://www.geeksforgeeks.org/java-8-predicate-with-examples/
*
 */
public class PredicateInterfaceExample {

	@Test
	public void example1() {
		// Creating predicate 
        Predicate<Integer> lesserthan = i -> (i < 18);  
        
        // Calling Predicate method 
        System.out.println(lesserthan.test(10));
	}
	
	@Test
	public void example2() {
		Predicate<Integer> greaterThanTen = (i) -> i > 10; 
		  
        // Creating predicate 
        Predicate<Integer> lowerThanTwenty = (i) -> i < 20;  
        boolean result = greaterThanTen.and(lowerThanTwenty).test(15); 
        System.out.println(result); 
  
        // Calling Predicate method 
        boolean result2 = greaterThanTen.and(lowerThanTwenty).negate().test(15); 
        System.out.println(result2);
	}
}
