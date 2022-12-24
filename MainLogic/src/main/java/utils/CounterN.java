package utils;

public class CounterN {
    private long N;

    private static class singletonHolder {
        private static CounterN instance = new CounterN();
    }

    public static CounterN getInstance() {
        return singletonHolder.instance;
    }

    private CounterN() {
        N = 0;
    }

    public long getN() {
        return N;
    }

    public void setN(long n) {
        N = n;
    }

}
