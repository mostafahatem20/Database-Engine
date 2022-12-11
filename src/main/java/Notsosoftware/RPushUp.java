package Notsosoftware;

public class RPushUp {

	/**
	 * This class is used for push keys up to the inner nodes in case
	 * of splitting at a lower level
	 */
	RTreeNode newNode;
	Double key;
	
	public RPushUp(RTreeNode newNode, Double key)
	{
		this.newNode = newNode;
		this.key = key;
	}
}
