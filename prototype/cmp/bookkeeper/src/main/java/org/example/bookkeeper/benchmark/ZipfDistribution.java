package org.example.bookkeeper.benchmark;

import java.util.Random;

/**
 * Zipf distribution implementation 
 */
public class ZipfDistribution {
    private final double exponent;
    private final double n;
    private final double hIntegralX1;
    private final double hIntegralXN;
    private final double s;
    private final Random random;

    /**
     * Create a new Zipf distribution.
     *
     * @param n The number of elements (samples will be in range [1, n])
     * @param exponent The Zipf exponent (higher values = more skewed towards 1)
     */
    public ZipfDistribution(double n, double exponent, Random random) {
        if (n <= 0) {
            throw new IllegalArgumentException("n must be positive");
        }
        if (exponent <= 0) {
            throw new IllegalArgumentException("exponent must be positive");
        }

        this.n = n;
        this.exponent = exponent;
        this.random = random;
        this.s = exponent;

        // Precompute values for rejection-inversion sampling
        this.hIntegralX1 = hIntegral(1.5) - 1.0;
        this.hIntegralXN = hIntegral(n + 0.5);
    }

    /**
     * Sample a value from the Zipf distribution.
     * @return An integer value in the range [1, n]
     */
    public int sample() {
        while (true) {
            double u = hIntegralXN + random.nextDouble() * (hIntegralX1 - hIntegralXN);
            double x = hIntegralInv(u);

            int k = (int) (x + 0.5);
            if (k < 1) {
                k = 1;
            } else if (k > n) {
                k = (int) n;
            }

            // Accept/reject based on the exact probability
            if (k - x <= s
                || u >= hIntegral(k + 0.5) - h(k)) {
                return k;
            }
        }
    }

    /**
     * Helper function h(x) = 1 / x^s
     */
    private double h(double x) {
        return Math.exp(-s * Math.log(x));
    }

    /**
     * Integral of h(x) from 0.5 to x
     */
    private double hIntegral(double x) {
        double logX = Math.log(x);
        return helper2((1.0 - s) * logX) * logX;
    }

    /**
     * Inverse of hIntegral
     */
    private double hIntegralInv(double x) {
        double t = x * (1.0 - s);
        if (t < -1.0) {
            // For very small x, use approximation
            t = -1.0;
        }
        return Math.exp(helper1(t) * x);
    }

    /**
     * Helper function for hIntegral
     */
    private double helper1(double x) {
        if (Math.abs(x) > 1e-8) {
            return Math.log1p(x) / x;
        } else {
            return 1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x));
        }
    }

    /**
     * Helper function for hIntegral
     */
    private double helper2(double x) {
        if (Math.abs(x) > 1e-8) {
            return Math.expm1(x) / x;
        } else {
            return 1.0 + x * (0.5 + x * (1.0 / 6.0 + x * (1.0 / 24.0)));
        }
    }
}
