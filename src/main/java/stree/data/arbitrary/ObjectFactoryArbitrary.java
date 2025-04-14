package stree.data.arbitrary;

import stree.data.ProductGraph;
import stree.engine.AbstractTRDExpansionJob;
import stree.data.Delta;
import stree.data.ObjectFactory;
import stree.data.ResultPair;
import stree.engine.RAPQTRDWithEdgeExpansionJob;
import stree.query.Automata;

public class ObjectFactoryArbitrary<V> implements ObjectFactory<V, TRDRAPQ<V>, TRDNodeRAPQ<V>> {
    @Override
    public TRDNodeRAPQ<V> createTRDNode(TRDRAPQ<V> tree, V vertex, int state) {
        return new TRDNodeRAPQ<V>(vertex, state);
    }

    @Override
    public TRDRAPQ<V> createTRD(Delta<V, TRDRAPQ<V>, TRDNodeRAPQ<V>> delta, V vertex, int state, V root) {
        return new TRDRAPQ<V>(delta, vertex, state, root);
    }

    @Override
    public <L> AbstractTRDExpansionJob createExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, ResultPair results,
                                                          long windowsize, long step, boolean isDeletion) {
        return new RAPQTRDWithEdgeExpansionJob<>(productGraph, automata, results, windowsize, step, isDeletion);
    }
}
