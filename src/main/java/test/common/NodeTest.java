package test.common;

import com.alibaba.fastjson.JSONArray;
import test.common.Node;

import java.util.ArrayList;
import java.util.List;
//二叉树遍历
public class NodeTest {
    public static void main(String[] args) {
        //tree key初始化
        Node node1=new Node();
        node1.setKey(3);
        Node node2=new Node();
        node2.setKey(9);
        Node node3=new Node();
        node3.setKey(20);
        Node node4=new Node();
        node4.setKey(15);
        Node node5=new Node();
        node5.setKey(7);
        //LR初始化
        node1.setL(node2);
        node1.setR(node3);
        node3.setL(node4);
        node3.setR(node5);

        List resarr=new ArrayList();
        //返回自底向上的函数
        lowToHigth(node1,resarr);
        resarr.add(new int[]{node1.getKey()});
        JSONArray jsonarr=new JSONArray(resarr);
        System.out.println(jsonarr.toString());

    }

    public static void   getLowChild (Node noderoot,List arr,int i){

       Node l =noderoot.getL();
       Node r =noderoot.getR();

       if(l==null&&r==null){
       }else{
           Node ll=l.getL();
           Node lr=l.getR();

           Node rl=r.getL();
           Node rr=r.getR();
           if(rl==null && rr==null && ll==null && lr==null){
               int[] itemarr= new int[]{l.getKey(), r.getKey()};
               arr.add(itemarr);
               i++;
               noderoot.setR(null);
               noderoot.setL(null);
           }
           if(l!=null){
               getLowChild(l,arr,i);
           }
           if(r!=null){
               getLowChild(r,arr,i);
           }
       }
    }

    public static void   lowToHigth(Node noderoot,List resarr){
        Node l =noderoot.getL();
        Node r =noderoot.getR();
        List arr=new ArrayList();
        int i=0;
        if(l==null&&r==null){
        }else{
            getLowChild(noderoot,arr,i);
            resarr.add(arr);
            lowToHigth(noderoot,resarr);
        }

    }
}
