import java.util.Random;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

/**
 * Helper function h(x) = 1 / x^s
 */
class ZipfDistribution {
    private final double exponent;
    private final double n;
    private final double hIntegralX1;
    private final double hIntegralXN;
    private final double s;
    private final Random random;

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

            if (k - x <= s || u >= hIntegral(k + 0.5) - h(k)) {
                return k;
            }
        }
    }

    private double h(double x) {
        return Math.exp(-s * Math.log(x));
    }

    private double hIntegral(double x) {
        double logX = Math.log(x);
        return helper2((1.0 - s) * logX) * logX;
    }

    private double hIntegralInv(double x) {
        double t = x * (1.0 - s);
        if (t < -1.0) {
            t = -1.0;
        }
        return Math.exp(helper1(t) * x);
    }

    private double helper1(double x) {
        if (Math.abs(x) > 1e-8) {
            return Math.log1p(x) / x;
        } else {
            return 1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x));
        }
    }

    private double helper2(double x) {
        if (Math.abs(x) > 1e-8) {
            return Math.expm1(x) / x;
        } else {
            return 1.0 + x * (0.5 + x * (1.0 / 6.0 + x * (1.0 / 24.0)));
        }
    }
}

/**
 * Standalone program to test Zipf distribution
 * Usage: javac ZipfTest.java && java ZipfTest
 */
public class ZipfTest {
    public static void main(String[] args) {
        double n = 100.0; // Range [1, 100]
        int sampleCount = 1000;
        double[] exponents = {0.5, 1.0, 1.5};
        Random random = new Random(42); // Fixed seed for reproducibility

        for (double exponent : exponents) {
            String filename = String.format("/tmp/java-%.1f.csv", exponent);

            try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                // Write CSV header
                writer.println("sample_num,value");

                ZipfDistribution zipf = new ZipfDistribution(n, exponent, random);

                for (int i = 0; i < sampleCount; i++) {
                    int value = zipf.sample();
                    writer.println(i + "," + value);
                }

                System.out.println("Generated " + filename);
            } catch (IOException e) {
                System.err.println("Error writing file " + filename + ": " + e.getMessage());
            }
        }
    }
}
