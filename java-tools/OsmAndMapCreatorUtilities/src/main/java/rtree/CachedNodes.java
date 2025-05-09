//BufferHeader.java,CachedNodes.java
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
package rtree;
//package rtree;
import java.util.*;

import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.*;
import rtree.seeded.SdNode;
/**
   <b>A circular linked list of cached nodes using hashtable ?!!</b>
   <p>This class will wrap a list of recently used nodes.
   If the requested node is in <tt>Hashtable</tt> of the class then the node would
   be returned from the <tt>Hashtable</tt>, else it would be read from the
   disk.
   <br>This will be a static object in the class RTree. Therefore no matter how
   many RTree objects you create they all would have one cache for all the files
   the object handles.
   TODO : keep a set that keeps all the nodes that are dirty. With each node registering themselves
   hara when thhey are dity, this is maageable.
   @author Prachuryya Barua
*/
class BufferHeader
{
  long recent;//the most recently inserted node
  long last;//the least recently inserted node
  int size;//max size of the link list
  TLongObjectHashMap<NodeValue> cache;
//  Hashtable cache;
  
  BufferHeader(int size,TLongObjectHashMap<NodeValue> ch)
  {
    this.cache = ch;
    this.size = size;
  }

  void flush()
    throws NodeWriteException
  {
    //if(!RTree.writeThr){
    for (Iterator<NodeValue> it = cache.valueCollection().iterator(); it.hasNext();){
      NodeValue node = it.next();
      node.node.flush();
    } // end of for (Iterator  = .iterator(); .hasNext();)
    //}
  }
  /*for a fresh key when the array is not full.This will be called when the
    buffer is not fully warm*/
  void put(long key, Node node)
  {
    try{
      //the following two conditions  happens in multithreaded programs
      if(cache.containsKey(key)){
        update(key);
        return;
      }
      if(cache.size() == size){
        replace(key,node);
        return;
      }
      if(cache.size() == 0){
        last = key;
        cache.put(key, new NodeValue(node,key,key));
      }else{
        //remove recent
        NodeValue tmpPrev = cache.remove(recent);
        if(last == recent){//there is only one node in cache
          cache.put(key, new NodeValue(node,tmpPrev.next,tmpPrev.next));
          cache.put(recent, new NodeValue(tmpPrev.node,key,key));
        }
        else{
          //remove next of previous
          NodeValue tmpPNext = cache.remove(tmpPrev.next);
          cache.put(key, new NodeValue(node,tmpPrev.next,recent));
          cache.put(tmpPrev.next, new NodeValue(tmpPNext.node,tmpPNext.next,key));
          cache.put(recent, new NodeValue(tmpPrev.node,key,tmpPrev.prev));
        }
      }
      recent = key;
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  /*a new key in a filled array*/
  void replace(long key,Node node)
    throws NodeWriteException
  {
    try{
      if(cache.containsKey(key)){
        update(key);
        return;
      }
      if(cache.size() < size){
        put(key,node);
        return;
      }
      //remove the 'last' node
      NodeValue lastNode = cache.remove(last);
      lastNode.node.flush();
      NodeValue pNode = cache.remove(lastNode.prev);
      NodeValue nNode = cache.remove(lastNode.next);
      //put back the three nodes
      cache.put(key, new NodeValue(node,lastNode.next, lastNode.prev));
      cache.put(lastNode.prev, new NodeValue(pNode.node,key,pNode.prev));
      cache.put(lastNode.next, new NodeValue(nNode.node,nNode.next,key));
      recent = key;//this is the latest node
      last = lastNode.next;//set the next in chain as the new 'last'

    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  /**make a node that is present as 'recent'*/
  void update(long key)
  {
    try{
      if(key != recent){
        NodeValue node,nextNode,prevNode,rcntNode,lastNode;//temp variables
        node = cache.remove(key);
        if(node == null){//will not happen
          System.out.println("CachedNodes.update: unlikely flow");
          return;
        }
        if(key == last){
          last = node.next;
          cache.put(key, new NodeValue(node.node, node.next, node.prev));
        }
        else{
          //adjust next node and the recent node
          nextNode = cache.remove(node.next);
          if(recent != node.next){//if next node is not the recent node
            rcntNode = cache.remove(recent);
            rcntNode.next = key;
            cache.put(recent, new NodeValue(rcntNode.node, rcntNode.next,rcntNode.prev));
          }
          else{//next node is the recent node
            nextNode.next = key;
          }
          cache.put(node.next, new NodeValue(nextNode.node, nextNode.next,node.prev));

          //adjust previous node and the last node - if unequal
          prevNode = cache.remove(node.prev);
          if(last != node.prev){//if last node is not the prev node
            lastNode = cache.remove(last);
            lastNode.prev = key;
            cache.put(last,new NodeValue(lastNode.node, lastNode.next,lastNode.prev));
          }
          else{//if the last node is the prev node.
            prevNode.prev = key;
          }
          cache.put(node.prev, new NodeValue(prevNode.node, node.next,prevNode.prev));

          //put the new node
          cache.put(key, new NodeValue(node.node,last,recent));
        }
        //update local variables
      }
      recent = key;
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  void remove(long key)
    throws NodeWriteException
  {
    try{
      NodeValue node = cache.remove(key);
      if((cache.size() != 0) && (node != null)){
        //if(!RTree.writeThr)
        node.node.flush();
        if(cache.size() == 1){
          NodeValue oNode = cache.remove(node.prev);
          cache.put(node.prev, new NodeValue(oNode.node, node.prev,node.prev));
          recent = last = node.prev;
        }
        else{
          //if(!RTree.writeThr)
          node.node.flush();
          NodeValue pNode = cache.remove(node.prev);
          NodeValue nNode = cache.remove(node.next);
          cache.put(node.prev, new NodeValue(pNode.node, node.next,pNode.prev));
          cache.put(node.next, new NodeValue(nNode.node, nNode.next,node.prev));
          if(key == recent)
            recent = node.prev;
          if(key == last)
            last = node.next;
        }
      }
    }
    catch(Exception e){
      e.printStackTrace();
    }
  }
  void reset()
    throws NodeWriteException
  {
    //if(!RTree.writeThr)
    flush();
    cache.clear();
  }
}
public class CachedNodes
{
  private static final int NODE = 0;
  private static final int SDNODE = 1;
  public static final int MAX_CACHE_SHIFT = 13;
//  Hashtable cache;
  TLongObjectHashMap<NodeValue> cache;
  BufferHeader buffHeader;
  int size = Node.CACHE_SIZE;
  CachedNodes()
  {
    //System.out.println("CachedNodes : cache called");
//    cache = new Hashtable(Node.CACHE_SIZE+1,1);
	  cache = new TLongObjectHashMap<NodeValue>(Node.CACHE_SIZE+1, 1);
    buffHeader = new BufferHeader(Node.CACHE_SIZE,cache);
    size = Node.CACHE_SIZE;
  }
  /**
     This one is still under construction.
  */
  CachedNodes(int size)
  {
    if(size < 0)
      throw new IllegalArgumentException("CachedNodes:: size is less than zero");
//    cache = new Hashtable(size+1,1);
    cache = new TLongObjectHashMap<NodeValue>(size+1, 1);
    buffHeader = new BufferHeader(size, cache);
    this.size = size;
  }
  public synchronized void setCacheSize(int size)
    throws NodeWriteException
  {
    if(size < 0)
      throw new IllegalArgumentException("CachedNodes:: size is less than zero");
    removeAll();
//    cache = new Hashtable(size+1,1);
    cache = new TLongObjectHashMap<NodeValue>(size+1, 1);
    buffHeader = new BufferHeader(size, cache);
    this.size = size;
  }
  public synchronized int getSize()
  {
    return cache.size();
  }
  private Node getNode(RandomAccessFile file,String fileName,long lndIndex,FileHdr flHdr, int type)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    long key = calKey(fileName,lndIndex);
    NodeValue node = cache.get(key);
    Node nNode;
    if(node == null){//Node not in cache
      if(type == NODE){
        nNode = new Node(file, fileName, lndIndex, flHdr);
      }else{
        nNode = new SdNode(file, fileName, lndIndex, flHdr);
      }
      key = calKey(fileName, (int)nNode.getNodeIndex());//this is for the case where index is NOT_DEFINED
      nNode.sweepSort();
      //cache not full
      if(cache.size() < Node.CACHE_SIZE){
        buffHeader.put(key,nNode);//(Node)nNode.clone());
      }else if(cache.size() == Node.CACHE_SIZE){//cache Is full
        buffHeader.replace(key,nNode);//(Node)nNode.clone());
      }
      return nNode;
    }
    else{//node found in the cache
      buffHeader.update(key);
      node.node.sweepSort();
      return (node.node);
    }
  }
  private Node getNode(RandomAccessFile file,String fileName,long parentIndex, int elmtType, FileHdr flHdr,
                       int type)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    Node nNode;
    if(type == NODE){
      nNode = new Node(file,fileName,parentIndex, elmtType, flHdr);
    }else{
      nNode = new SdNode(file,fileName,parentIndex, elmtType, flHdr);
    }
    long key = calKey(fileName, nNode.getNodeIndex());
    nNode.sweepSort();
    //cache not full
    if(cache.size() < Node.CACHE_SIZE)
      buffHeader.put(key,nNode);
    //cache Is full
    else if(cache.size() == Node.CACHE_SIZE)
      buffHeader.replace(key,nNode);
    return nNode;
  }
  //-----------------------Methods for client to get Node they prefer------------
  /**
     This one returns an existing <code>SdNode</code>
  */
  public synchronized SdNode getSdNode(RandomAccessFile file,String fileName,long lndIndex,FileHdr flHdr)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    return (SdNode)getNode(file,fileName,lndIndex,flHdr, SDNODE);
  }
  /**
     This one returns an existing <code>SdNode</code>.
  */
  public synchronized SdNode getSdNode(RandomAccessFile file,String fileName,long parentIndex,
                                       int elmtType, FileHdr flHdr)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    return (SdNode)getNode(file,fileName,parentIndex, elmtType, flHdr, SDNODE);
  }
  /**
     This one returns an existing <code>Node</code>.
  */
  public synchronized Node getNode(RandomAccessFile file,String fileName,long lndIndex,FileHdr flHdr)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    return getNode(file,fileName,lndIndex,flHdr, NODE);
  }
  /**
     This one returns an new <code>SdNode</code>.
  */
  public synchronized Node getNode(RandomAccessFile file,String fileName,long parentIndex,
                                   int elmtType, FileHdr flHdr)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    return getNode(file,fileName,parentIndex, elmtType, flHdr, NODE);
  }
  /**
     This one returns an new <code>Node</code>.
  */
  public synchronized Node getNode(RandomAccessFile file,String fileName,long parentIndex,
                                   int elmtType, FileHdr flHdr, Node type)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    if(type instanceof SdNode)
      return getNode(file,fileName,parentIndex, elmtType, flHdr, SDNODE);
    else
      return getNode(file,fileName,parentIndex, elmtType, flHdr, NODE);
  }
  /**
     This one return <code>ReadNode</code> a read only node.
     All clients that need only to query the rtree must at all cost call this method. This method will
     return a clones ReadNode, so that concurrent reads can take place (because none of the methods
     of <code>Node</code> are <code>synchronized</code>.
  */
  public synchronized ReadNode getReadNode(RandomAccessFile file,String fileName,long lndIndex,FileHdr flHdr)
    throws IllegalValueException, NodeReadException, FileNotFoundException, IOException, NodeWriteException
  {
    return ReadNode.makeReadNode(getNode(file,fileName,lndIndex,flHdr));
  }
  /**
     Write all the diry nodes to the disc.
  */
  synchronized void flush()
    throws NodeWriteException
  {
    //if(!RTree.writeThr)
    buffHeader.flush();
  }
  /**
     This method would be called only by those threads that need to modify the
     tree. Hence this method is automatically synchronized.
  */
  synchronized void remove(String fileName,long ndIndex)
    throws NodeWriteException
  {
    long key = calKey(fileName,ndIndex);
    buffHeader.remove(key);
  }


  synchronized void removeAll()
    throws NodeWriteException
  {
    buffHeader.reset();
  }

  static Map<String, Integer> fileNamesMap = new LinkedHashMap<String, Integer>();

  static void clearFileNamesMap(){
	  fileNamesMap.clear();
  }

  synchronized long calKey(String fileName,long idx)
  {
    if(fileName != null) {
		if (idx < -(1l << (62l - MAX_CACHE_SHIFT))) {
			throw new IllegalArgumentException("Id underflow: " + idx);
		}
		if (idx > (1l << (62l - MAX_CACHE_SHIFT))) {
			throw new IllegalArgumentException("Id overflow: " + idx);
		}
    	Integer i = fileNamesMap.get(fileName);
    	if(i == null){
    		if(fileNamesMap.size() >= (1 << MAX_CACHE_SHIFT)){
    			throw new ArrayIndexOutOfBoundsException();
    		}
    		fileNamesMap.put(fileName, fileNamesMap.size());
    		i = fileNamesMap.get(fileName);
    	}
//      System.out.println(idx + " " + fileName + " " + ((idx << 5)+ fileName.toLowerCase().hashCode() % 32));
      return ((idx<< MAX_CACHE_SHIFT)+ i);
    } else{
      System.out.println("CachedNodes.calKey: fileName null");
      return 0;
    }
  }
}
class NodeValue
{
  Node node;
  long next;//the next node's key
  long prev;//the prev node's key
  NodeValue(Node node,long n,long p)
  {
    this.node = node;
    next = n;
    prev = p;
  }
}
/**
   TODO:
   2) A way to pin an node. Obviously the client must also unpinn the node.

*/
