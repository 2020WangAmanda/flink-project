package com.wangran.firstflink;

public class ItemViewCount {
     public  long itemId;
     public long windowEnd;
     public long viewCount;

     public static  ItemViewCount of(long itemId, long windowEnd,long viewCount ){
         ItemViewCount result =new ItemViewCount();
         result.itemId =itemId;
         result.windowEnd =windowEnd;
         result.viewCount =viewCount;
         return  result;


     }
}
