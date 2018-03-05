package utils;

import org.locationtech.jts.index.ItemVisitor;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Jacob on 23.01.2017.
 */
public class InvertavlTreeVisitor implements ItemVisitor {


    private List<Object> visitedItems = new ArrayList<>();


    @Override
    public void visitItem(Object item) {
       // System.out.println("visited object: " + item);


        visitedItems.add(item);
    }

    public List getVisitedItems(){
        return visitedItems;
    }
}
