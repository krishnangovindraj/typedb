/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.common.iterator;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.common.iterator.sorted.MappedSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.MergeMappedSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.parameters.Order;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public abstract class AbstractFunctionalIterator<T> implements FunctionalIterator<T> {

    @Override
    public FunctionalIterator<T> distinct() {
        return new DistinctIterator<>(this);
    }

    @Override
    public FunctionalIterator<T> distinct(Set<T> duplicates) {
        return new DistinctIterator<>(this, duplicates);
    }

    @Override
    public <U> FunctionalIterator<U> map(Function<T, U> mappingFn) {
        return new MappedIterator<>(this, mappingFn);
    }

    @Override
    public <U extends Comparable<? super U>, ORDER extends Order> SortedIterator<U, ORDER> mapSorted(Function<T, U> mappingFn, ORDER order) {
        return new MappedSortedIterator<>(this, mappingFn, order);
    }

    @Override
    public <U> FunctionalIterator<U> flatMap(Function<T, FunctionalIterator<U>> mappingFn) {
        return new FlatMappedIterator<>(this, mappingFn);
    }

    @Override
    public <U extends Comparable<? super U>, ORDER extends Order> SortedIterator<U, ORDER> mergeMap(
            Function<T, SortedIterator<U, ORDER>> mappingFn, ORDER order
    ) {
        return new MergeMappedSortedIterator<>(this, mappingFn, order);
    }

    @Override
    public <U extends Comparable<? super U>, ORDER extends Order> Forwardable<U, ORDER> mergeMapForwardable(
            Function<T, Forwardable<U, ORDER>> mappingFn, ORDER order
    ) {
        return new MergeMappedSortedIterator.Forwardable<>(this, mappingFn, order);
    }

    @Override
    public FunctionalIterator<T> filter(Predicate<T> predicate) {
        return new FilteredIterator<>(this, predicate);
    }

    @Override
    public FunctionalIterator<T> offset(long offset) {
        return new OffsetIterator<>(this, offset);
    }

    @Override
    public FunctionalIterator<T> limit(long limit) {
        return new LimitedIterator<>(this, limit);
    }

    @Override
    public FunctionalIterator<T> link(FunctionalIterator<T> iterator) {
        return new LinkedIterators<>(list(this, iterator));
    }

    @Override
    public FunctionalIterator<T> link(Iterator<T> iterator) {
        if (iterator instanceof AbstractFunctionalIterator<?>) return link((FunctionalIterator<T>) iterator);
        return new LinkedIterators<>(list(this, iterate(iterator)));
    }

    @Override
    public FunctionalIterator<T> noNulls() {
        return this.filter(Objects::nonNull);
    }

    @Override
    public boolean allMatch(Predicate<T> predicate) {
        boolean match = !filter(e -> !predicate.test(e)).hasNext();
        recycle();
        return match;
    }

    @Override
    public boolean anyMatch(Predicate<T> predicate) {
        boolean match = filter(predicate).hasNext();
        recycle();
        return match;
    }

    @Override
    public boolean noneMatch(Predicate<T> predicate) {
        return !anyMatch(predicate);
    }

    @Override
    public Optional<T> first() {
        return Optional.ofNullable(firstOrNull());
    }

    @Override
    public T firstOrNull() {
        T next = hasNext() ? next() : null;
        recycle();
        return next;
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(
                spliteratorUnknownSize(this, ORDERED | IMMUTABLE), false
        ).onClose(this::recycle);
    }

    @Override
    public List<T> toList() {
        ArrayList<T> list = new ArrayList<>();
        forEachRemaining(list::add);
        recycle();
        return list;
    }

    @Override
    public List<List<T>> toLists(int split) {
        List<List<T>> lists = new ArrayList<>(split);
        for (int i = 0; i < split; i++) lists.add(new ArrayList<>());
        int i = 0;
        while (hasNext()) {
            lists.get(i).add(next());
            i++;
            if (i == split) i = 0;
        }
        return lists;
    }

    @Override
    public List<List<T>> toLists(int minSize, int maxSplit) {
        assert minSize > 0 && maxSplit > 0;
        List<List<T>> lists = new ArrayList<>(maxSplit);

        if (!hasNext()) {
            lists.add(new ArrayList<>());
            return lists;
        }

        for (int i = 0; i < maxSplit && hasNext(); i++) {
            List<T> list = new ArrayList<>();
            for (int j = 0; j < minSize && hasNext(); j++) list.add(next());
            lists.add(list);
        }
        int i = 0;
        while (hasNext()) {
            lists.get(i).add(next());
            i++;
            if (i == maxSplit) i = 0;
        }
        return lists;
    }

    @Override
    public void toList(List<T> list) {
        forEachRemaining(list::add);
        recycle();
    }

    @Override
    public Set<T> toSet() {
        HashSet<T> set = new HashSet<>();
        this.forEachRemaining(set::add);
        return set;
    }

    @Override
    public void toSet(Set<T> set) {
        this.forEachRemaining(set::add);
        recycle();
    }

    @Override
    public <U extends Collection<? super T>> U collect(Supplier<U> constructor) {
        U collection = constructor.get();
        this.forEachRemaining(collection::add);
        return collection;
    }

    @Override
    public long count() {
        long count = 0;
        for (; hasNext(); count++) next();
        recycle();
        return count;
    }

    @Override
    public <ACC> ACC reduce(ACC initial, BiFunction<T, ACC, ACC> accumulate) {
        ACC acc = initial;
        while (hasNext()) {
            T value = next();
            acc = accumulate.apply(value, acc);
        }
        return acc;
    }

    @Override
    public FunctionalIterator<T> onConsumed(Runnable function) {
        return new ConsumeHandledIterator<>(this, function);
    }

    @Override
    public FunctionalIterator<T> onError(Function<Exception, TypeDBException> exceptionFn) {
        return new ErrorHandledIterator<>(this, exceptionFn);
    }

    @Override
    public FunctionalIterator<T> onFinalise(Runnable function) {
        return new FinaliseIterator<>(this, function);
    }

    @Override
    public abstract void recycle();

}
