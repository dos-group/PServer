package de.tuberlin.pserver.crdt;

import de.tuberlin.pserver.crdt.operations.Operation;

/**
 * <p>
 * The root interface in the CRDT hierarchy. This interface is meant to be implemented by classes that represent
 * Operation-Based Commutative Replicated Data Types as defined by Shapiro et al. (see below). This library offers no
 * direct implementations of the CRDT interface but instead provides implementations of more specific sub-interfaces.
 *</p>
 * <p>
 * Marc Shapiro, Nuno Preguyca, Carlso Baquero, Marek Zawirski. A comprehensive study of Convergent and Commutative
 * Replicated Data Types. [Research Report] RR-7506, Inria - Centre Paris-Rocquencourt; INRIA. 2011, pp.50.)
 * </p>
 */

public interface CRDT<T> {

}