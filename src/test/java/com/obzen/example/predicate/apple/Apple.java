package com.obzen.example.predicate.apple;

public class Apple {
	private String color;
	private int weight;

	public Apple(String color, int weight) {
		this.color = color;
		this.weight = weight;
	}
	
	public String getColor() {
		return color;
	}

	public int getWeight() {
		return weight;
	}

	public static boolean isGreenApple(Apple apple) {
		return AppleColor.GREEN.getColor().equals(apple.getColor());
	}

	public static boolean isHeavyApple(Apple apple) {
		return apple.getWeight() > 150;
	}
}
