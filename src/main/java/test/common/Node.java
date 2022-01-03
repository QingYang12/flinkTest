package test.common;

public class Node {
    int key;
    Node L;
    Node R;

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public Node getL() {
        return L;
    }

    public void setL(Node l) {
        L = l;
    }

    public Node getR() {
        return R;
    }

    public void setR(Node r) {
        R = r;
    }
}
