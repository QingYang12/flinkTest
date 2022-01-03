package test.test;

public class Test2 {
    public static void main(String[] args) {
        int[][] arr={};
        int[] itemarr= new int[]{1, 2};
        arr[0][0]=itemarr[0];
        arr[0][1]=itemarr[1];

        System.out.println(String.valueOf(arr).toString());
    }
}
