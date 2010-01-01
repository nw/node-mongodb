#include <v8.h>
#include <node.h>

extern "C" {
    #define MONGO_HAVE_STDINT
    #include <bson.h>
}

using namespace v8;

const char *
ToCString(const String::Utf8Value& value) {
  return *value ? *value : "<string conversion failed>";
}

inline void
encodeString(bson_buffer *bb, const char *name, const Local<Value> element) {
    String::Utf8Value v(element);
    const char *value(ToCString(v));
    bson_append_string(bb, name, value);
}

inline void
encodeNumber(bson_buffer *bb, const char *name, const Local<Value> element) {
    double value(element->NumberValue());
    bson_append_double(bb, name, value);
}

inline void
encodeInteger(bson_buffer *bb, const char *name, const Local<Value> element) {
    int value(element->NumberValue());
    bson_append_int(bb, name, value);
}

inline void
encodeBoolean(bson_buffer *bb, const char *name, const Local<Value> element) {
    bool value(element->IsTrue());
    bson_append_bool(bb, name, value);
}

bson encodeObject(const Local<Value> element) {
    HandleScope scope;
    bson_buffer bb;
    bson_buffer_init(&bb);

    Local<Object> object = element->ToObject();
    Local<Array> properties = object->GetPropertyNames();

    for (int i = 0; i < properties->Length(); i++) {
        // get the property name and value
        Local<Value> prop_name = properties->Get(Integer::New(i));
        Local<Value> prop_val = object->Get(prop_name->ToString());

        // convert the property name to a c string
        String::Utf8Value n(prop_name);
        const char *pname = ToCString(n);
       
        // append property using appropriate appender
        if (prop_val->IsString()) {
            encodeString(&bb, pname, prop_val);
        }
        else if (prop_val->IsInt32()) {
            encodeInteger(&bb, pname, prop_val);
        }
        else if (prop_val->IsNumber()) {
            encodeNumber(&bb, pname, prop_val);
        }
        else if (prop_val->IsBoolean()) {
            encodeBoolean(&bb, pname, prop_val);
        }
        else if (prop_val->IsObject()) {
            bson bson(encodeObject(prop_val));
            bson_append_bson(&bb, pname, &bson);
        }
    }

    bson bson;
    bson_from_buffer(&bson, &bb);
    return bson;
}

Handle<Value>
encode(const Arguments &args) {
    // TODO assert args.length > 0
    // TODO assert args.type == Object
    HandleScope scope;

    bson bson(encodeObject(args[0]));

    return node::Encode(bson.data, bson_size(&bson), node::BINARY);
}

Local<Value>
decodeObjectStr(const char *buf) {
    HandleScope scope;

    bson_iterator it;
    bson_iterator_init(&it, buf);
    Local<Object> obj = Object::New();

    while (bson_iterator_next(&it)) {
        bson_type type = bson_iterator_type(&it);
        const char *key = bson_iterator_key(&it);
        //fprintf(stderr, "key was %s\n", key);

        switch (type) {
            case bson_string: 
                {
                    const char *val = bson_iterator_string(&it);
                    obj->Set(String::New(key), String::New(val));
                }
                break;

            case bson_int:
                {
                    int val = bson_iterator_int(&it);
                    obj->Set(String::New(key), Number::New(val));
                }
                break;

            case bson_double:
                {
                    double val = bson_iterator_double_raw(&it);
                    obj->Set(String::New(key), Number::New(val));
                }
                break;

            case bson_object:
                {
                    bson bson;
                    bson_iterator_subobject(&it, &bson);
                    Handle<Value> sub = decodeObjectStr(bson.data);
                    obj->Set(String::New(key), sub);
                }
                break;

            case bson_bool:
                {
                    bson_bool_t val = bson_iterator_bool(&it);
                    obj->Set(String::New(key), Boolean::New(val));
                }
                break;
        }
    }

    return scope.Close(obj);
}

Handle<Value>
decodeObject(const Local<Value> str) {
    HandleScope scope;
    size_t buflen = str->ToString()->Length();
    char buf[buflen];
    node::DecodeWrite(buf, buflen, str, node::BINARY);
    return decodeObjectStr(buf);
}

Handle<Value>
decode(const Arguments &args) {
    HandleScope scope;
    return decodeObject(args[0]);
}

// extern "C" void
// init (Handle<Object> target) {
//     HandleScope scope;
//     target->Set(
//         String::New("encode"),
//         FunctionTemplate::New(encode)->GetFunction());
//     target->Set(
//         String::New("decode"),
//         FunctionTemplate::New(decode)->GetFunction());
// }
