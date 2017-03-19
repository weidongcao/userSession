package com.don.session.util;

import java.util.Random;

/**
 * Created by Administrator on 2017/2/28.
 */
public class CreatePassword {
    public static void main(String[] args) {
        System.out.println("password = " + getPassword(10));
    }

    public static String getPassword(int length) {
        Random random = new Random();

        StringBuilder passwordCache = new StringBuilder();

        for (int i = 0; i < length; i++) {
            int randomInt = random.nextInt(62);
            if ((0 <= randomInt) && (10 > randomInt)) {
                randomInt += 48;
                passwordCache.append(((char) randomInt));
            } else if ((10 <= randomInt) && (36 > randomInt)) {
                randomInt += (65 - 10);
                passwordCache.append((char) randomInt);
            } else if ((36 <= randomInt) && (62 > randomInt)) {
                randomInt += (97 - 10 - 26);
                passwordCache.append((char) randomInt);
            }
        }

        return passwordCache.toString();
    }

}
