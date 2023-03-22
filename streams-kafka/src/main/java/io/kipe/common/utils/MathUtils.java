/*
 * Kipes SDK for Kafka - The High-Level Event Processing SDK.
 * Copyright © 2023 kipe.io
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package io.kipe.common.utils;

public class MathUtils {

	public static double round(double value, int digits) {
		double f = Math.pow(10, digits);
		return Math.round(value * f) / f; 
	}
	
	public static int getPrecision(double value) {
		double absValue = Math.abs(value);
		double positiveFractionalPart = absValue - Math.floor(absValue);
		
		if(positiveFractionalPart == 0.0) {
			return 0;
		}
		
		int precision = 1;
		while(precision < 17) { // max precision of double
			double poweredValue = absValue * Math.pow(10, precision);
			double poweredFractionalPart = poweredValue - Math.floor(poweredValue);
			if(poweredFractionalPart < 0.0000000000000001) {
				break;
			}
			
			precision += 1;
		}
		
		return precision;
	}
	
	private MathUtils() {}
}
