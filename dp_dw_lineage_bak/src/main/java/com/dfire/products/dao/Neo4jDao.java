package com.dfire.products.dao;

import java.util.List;

import com.dfire.products.bean.ColumnNode;
import com.dfire.products.bean.RealationShip;
import com.dfire.products.bean.TableNode;

public interface Neo4jDao {

	public int createTable(TableNode node);
	
	public int createColumn(long tableId, List<ColumnNode> list);
	
	public int createTableRealationShip(RealationShip ship);
	
	public int createColumnRealationShip(RealationShip ship);
	
}
