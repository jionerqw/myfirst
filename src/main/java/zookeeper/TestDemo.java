package zookeeper;


import java.util.ArrayList;

public class TestDemo {
    public static void main(String[] args) {
        //第一次状态server id =1
        ArrayList<Integer> a1 = new ArrayList<Integer>();
        //第二次状态server id =1,2
        ArrayList<Integer> a2 = new ArrayList<Integer>();
        a1.add(1);
        a1.add(2);
        a2.add(1);

        if(a2.size()>a1.size()){
            a2.removeAll(a1);
            System.out.println("服务器上线"+a2.get(0));
        }else{
            a1.removeAll(a2);
            System.out.println("服务器下线"+a1.get(0));
        }

    }
}