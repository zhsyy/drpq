package stree.data;

import stree.engine.AbstractTRDExpansionJob;
import stree.query.Automata;

public interface  ObjectFactory<V, T extends AbstractTRD<V, T, N>, N extends AbstractTRDNode<V, T, N>> {

    N createTRDNode(T tree, V vertex, int state);

    T createTRD(Delta<V, T, N> delta, V vertex, int state, V root);

    <L> AbstractTRDExpansionJob createExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, ResultPair results,
                                                   long windowsize, long step, boolean isDeletion);

}
