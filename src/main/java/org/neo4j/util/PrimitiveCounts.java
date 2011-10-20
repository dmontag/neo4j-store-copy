package org.neo4j.util;

import java.lang.reflect.Field;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class PrimitiveCounts
{
    public static void main( String[] args )
    {
        args = new String[] { "target/test-data/stuff" };
        GraphDatabaseService graphdb = new EmbeddedGraphDatabase( args[0] );
        PrimitiveCounts counts = new PrimitiveCounts();
        try
        {
            counts.run( graphdb );
        }
        finally
        {
            graphdb.shutdown();
            counts.report();
        }
    }

    private long nodes;
    private long rels;
    private long brokenRelChains;
    private long nodeProps;
    private long brokenNodePropChains;
    private long nullNodeProps;
    private long relProps;
    private long brokenRelPropChains;
    private long nullRelProps;

    private void run( GraphDatabaseService graphdb )
    {
        for ( Node node : graphdb.getAllNodes() )
        {
            nodes++;
            countProps( node, Kind.NODE );
            try
            {
                for ( Relationship rel : node.getRelationships() )
                {
                    rels++;
                    countProps( rel, Kind.REL );
                }
            }
            catch ( Throwable t )
            {
                brokenRelChains++;
                System.err.println( t );
            }
        }
    }

    private void countProps( PropertyContainer entity, Kind kind )
    {
        try
        {
            for ( String key : entity.getPropertyKeys() )
            {
                if ( entity.getProperty( key ) != null )
                    kind.addProp( this );
                else
                    kind.addNullProp( this );
            }
        }
        catch ( Throwable t )
        {
            kind.addBrokenPropChain( this );
            System.err.println( t );
        }
    }

    private void report()
    {
        for ( Field field : getClass().getDeclaredFields() )
        {
            try
            {
                log( field.getName(), ( (Number) field.get( this ) ).longValue() );
            }
            catch ( Exception e )
            {
                throw new Error( e );
            }
        }
    }

    private void log( String name, long value )
    {
        System.out.println( name + ": " + value );
    }

    private enum Kind
    {
        NODE
        {
            @Override
            void addProp( PrimitiveCounts counts )
            {
                counts.nodeProps++;
            }

            @Override
            void addBrokenPropChain( PrimitiveCounts counts )
            {
                counts.brokenNodePropChains++;
            }

            @Override
            void addNullProp( PrimitiveCounts counts )
            {
                counts.nullNodeProps++;
            }
        },
        REL
        {
            @Override
            void addProp( PrimitiveCounts counts )
            {
                counts.relProps++;
            }

            @Override
            void addBrokenPropChain( PrimitiveCounts counts )
            {
                counts.brokenRelPropChains++;
            }

            @Override
            void addNullProp( PrimitiveCounts counts )
            {
                counts.nullRelProps++;
            }
        };

        abstract void addProp( PrimitiveCounts counts );

        abstract void addBrokenPropChain( PrimitiveCounts counts );

        abstract void addNullProp( PrimitiveCounts counts );
    }
}
