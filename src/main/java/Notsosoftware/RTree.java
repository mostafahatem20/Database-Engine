package Notsosoftware;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class RTree implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private RTreeNode root;
	
	/**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public RTree(int order) 
	{
		this.order = order;
		root = new RTreeLeafNode (this.order);
		root.setRoot(true);
	}
	public RTreeNode getNodeGreater(Double key)
	{
		return root.getNodeGreater(key);
	}
	public RTreeNode getNodeGreaterE(Double key)
	{
		return root.getNodeGreaterE(key);
	}
	public RTreeLeafNode getFirstNode() {
		return root.getFirstNode();
	}
	
	
	public boolean deleteByRef(Ref ref) {
		boolean done = root.deleteByRef(ref, null, -1);
		// go down and find the new root in case the old root is deleted
		while (root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode ) root).getFirstChild();
		return done;
	}
	public void update4(Ref reference, int N) {

		root.setRef4(reference, N);
	}

	public void update5(Ref reference, int N) {
		root.setRef5(reference, N);
	}
	public void deletedPage(int PageNum) {
		root.deletedPage(PageNum);
	}
	/**
	 * Inserts the specified key associated with the given record in the B+ tree
	 * @param key the key to be inserted
	 * @param recordReference the reference of the record associated with the key
	 */
	public void insert(Double key, Vector<Ref> recordReference)
	{
		RPushUp  pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			RTreeInnerNode  newRoot = new RTreeInnerNode (order);
			newRoot.insertLeftAt(0, pushUp.key, root);
			newRoot.setChild(1, pushUp.newNode);
			root.setRoot(false);
			root = newRoot;
			root.setRoot(true);
		}
	}
	
	
	/**
	 * Looks up for the record that is associated with the specified key
	 * @param key the key to find its record
	 * @return the reference of the record associated with this key 
	 */
	public Vector<Ref> search(Double key)
	{
		return root.search(key);
	}
	
	public Vector searchPos(Double key)
	{
		return root.searchPos(key);
	}
	
	public void update(Double key, Vector<Ref> reference)
	{
		root.setRef(key, reference);
	}
	
	public void update2(Double key, Ref reference, int N)
	{

		root.setRef2(key, reference, N);
	}
	
	public void update3(Double key, Ref reference, int N)
	{
		root.setRef3(key, reference, N);
//		Ref r = new Ref (reference.getPage()+1, 0);
//		if (v.size()!=0) {
//			root.setRef3(v.get(0), r, N);
//		}
	}
	/**
	 * Delete a key and its associated record from the tree.
	 * @param key the key to be deleted
	 * @return a boolean to indicate whether the key is successfully deleted or it was not in the tree
	 */
	public boolean delete(Double key)
	{
		boolean done = root.delete(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode ) root).getFirstChild();
		return done;
	}
	
	public boolean delete(Double key, int PageNum, int IndexInPage)
	{
		boolean done = root.delete(key, null, -1, PageNum, IndexInPage);
		//go down and find the new root in case the old root is deleted
		while(root instanceof RTreeInnerNode && !root.isRoot())
			root = ((RTreeInnerNode ) root).getFirstChild();
		return done;
	}
///////////////////////////////////////////////////////////////////////	
	public RTreeNode  getNode(Double key)
	{
		return root.getNode(key);
	}
	
//////////////////////////////////////////////////////////////////////////	
	/**
	 * Returns a string representation of the B+ tree.
	 */
	public String toString()
	{	
		
		//	<For Testing>
		// node :  (id)[k1|k2|k3|k4]{P1,P2,P3,}
		String s = "";
		Queue<RTreeNode > cur = new LinkedList<RTreeNode >(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<RTreeNode >();
			while(!cur.isEmpty())
			{
				RTreeNode  curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof RTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					RTreeInnerNode  parent = (RTreeInnerNode ) curNode;
					for(int i = 0; i <= parent.numberOfKeys; ++i)
					{
						System.out.print(parent.getChild(i).index+",");
						next.add(parent.getChild(i));
					}
					System.out.print("} ");
				}
				
			}
			System.out.println();
			cur = next;
		}	
		//	</For Testing>
		return s;
	}
}
