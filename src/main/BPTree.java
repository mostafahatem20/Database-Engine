package Notsosoftware;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Vector;

public class BPTree<T extends Comparable<T>> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int order;
	private BPTreeNode<T> root;
	
	/**
	 * Creates an empty B+ tree
	 * @param order the maximum number of keys in the nodes of the tree
	 */
	public BPTree(int order) 
	{
		this.order = order;
		root = new BPTreeLeafNode<T>(this.order);
		root.setRoot(true);
	}
	public BPTreeNode<T> getNodeGreater(T key)
	{
		return root.getNodeGreater(key);
	}
	public BPTreeNode<T> getNodeGreaterE(T key)
	{
		return root.getNodeGreaterE(key);
	}
	public BPTreeLeafNode getFirstNode() {
		return root.getFirstNode();
	}
	
	
	public boolean deleteByRef(Ref ref) {
		boolean done = root.deleteByRef(ref, null, -1);
		// go down and find the new root in case the old root is deleted
		while (root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
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
	public void insert(T key, Vector<Ref> recordReference)
	{
		PushUp<T> pushUp = root.insert(key, recordReference, null, -1);
		if(pushUp != null)
		{
			BPTreeInnerNode<T> newRoot = new BPTreeInnerNode<T>(order);
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
	public Vector<Ref> search(T key)
	{
		return root.search(key);
	}
	
	public Vector searchPos(T key)
	{
		return root.searchPos(key);
	}
	
	public void update(T key, Vector<Ref> reference)
	{
		root.setRef(key, reference);
	}
	
	public void update2(T key, Ref reference, int N)
	{

		root.setRef2(key, reference, N);
	}
	
	public void update3(T key, Ref reference, int N)
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
	public boolean delete(T key)
	{
		boolean done = root.delete(key, null, -1);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
	
	public boolean delete(T key, int PageNum, int IndexInPage)
	{
		boolean done = root.delete(key, null, -1, PageNum, IndexInPage);
		//go down and find the new root in case the old root is deleted
		while(root instanceof BPTreeInnerNode && !root.isRoot())
			root = ((BPTreeInnerNode<T>) root).getFirstChild();
		return done;
	}
///////////////////////////////////////////////////////////////////////	
	public BPTreeNode<T> getNode(T key)
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
		Queue<BPTreeNode<T>> cur = new LinkedList<BPTreeNode<T>>(), next;
		cur.add(root);
		while(!cur.isEmpty())
		{
			next = new LinkedList<BPTreeNode<T>>();
			while(!cur.isEmpty())
			{
				BPTreeNode<T> curNode = cur.remove();
				System.out.print(curNode);
				if(curNode instanceof BPTreeLeafNode)
					System.out.print("->");
				else
				{
					System.out.print("{");
					BPTreeInnerNode<T> parent = (BPTreeInnerNode<T>) curNode;
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
